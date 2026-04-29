// OnboardingFlowTests.swift
// Verifies acceptance criteria for playhead-1v8 (first-launch onboarding flow).
//
// Behavioral coverage:
// - Persistence: completing onboarding sets `hasCompletedOnboarding`
//   and the flag survives a fresh UserDefaults handle (relaunch sim).
// - Skip: tapping Skip on any screen flips the same flag — no second
//   tour ever shows.
// - Search-prompt completion: "Get Started" sets the Browse tab hint
//   and ContentView consumes it exactly once.
// - Step machine: the three-step sequence advances welcome → valueProp
//   → searchPrompt; the terminal step's `next` is nil.
// - Verbatim copy pin: the value-prop body is the exact bead text.
//
// We test the view-model action handlers and pure persistence helpers,
// per playhead-1v8: "don't try to drive SwiftUI gestures."

import XCTest
@testable import Playhead

// MARK: - Helpers

private func makeDefaults() -> (UserDefaults, String) {
    let suiteName = "OnboardingFlowTests.\(UUID().uuidString)"
    let defaults = UserDefaults(suiteName: suiteName)!
    return (defaults, suiteName)
}

// MARK: - Step machine

final class OnboardingStepMachineTests: XCTestCase {

    func testWelcomeAdvancesToValueProp() {
        XCTAssertEqual(OnboardingStep.welcome.next, .valueProp)
    }

    func testValuePropAdvancesToSearchPrompt() {
        XCTAssertEqual(OnboardingStep.valueProp.next, .searchPrompt)
    }

    func testSearchPromptIsTerminal() {
        XCTAssertNil(
            OnboardingStep.searchPrompt.next,
            "Search-prompt is the last screen; tapping Get Started dismisses onboarding."
        )
    }

    func testAllCasesEnumerated() {
        // Pin the case set so a future addition forces a deliberate
        // update to this bead's tests rather than silently extending
        // the flow.
        XCTAssertEqual(OnboardingStep.allCases, [.welcome, .valueProp, .searchPrompt])
    }
}

// MARK: - Verbatim copy

final class OnboardingFlowCopyTests: XCTestCase {

    func testValuePropBodyIsVerbatim() {
        // Bead text, character-for-character.
        XCTAssertEqual(
            OnboardingFlowCopy.valuePropBody,
            "Your podcasts, without the ads. All on-device, all private."
        )
    }

    func testWelcomeWordmarkIsVerbatim() {
        XCTAssertEqual(OnboardingFlowCopy.welcomeWordmark, "Playhead")
    }

    func testSkipButtonLabel() {
        XCTAssertEqual(OnboardingFlowCopy.skipButton, "Skip")
    }

    func testSearchHeadlineNonEmpty() {
        // Headline copy is editorial and may evolve; pinning the exact
        // string here would invite churn. We just verify it isn't empty.
        XCTAssertFalse(OnboardingFlowCopy.searchHeadline.isEmpty)
        XCTAssertFalse(OnboardingFlowCopy.searchBody.isEmpty)
    }
}

// MARK: - Persistence keys

final class OnboardingPersistenceKeyTests: XCTestCase {

    func testHasCompletedOnboardingKeyMatchesAppStorage() {
        // RootView in PlayheadApp.swift uses `@AppStorage("hasCompletedOnboarding")`.
        // The constant here MUST match that literal — renaming it
        // silently re-shows onboarding to upgrading users.
        XCTAssertEqual(
            OnboardingFlags.hasCompletedOnboardingKey,
            "hasCompletedOnboarding"
        )
    }

    func testRequestedInitialTabKeyIsStable() {
        XCTAssertEqual(
            OnboardingFlags.requestedInitialTabKey,
            "onboardingRequestedInitialTab"
        )
    }

    func testInitialTabRawValuesAreStable() {
        // Raw values are persisted; a rename is a breaking change.
        XCTAssertEqual(OnboardingInitialTab.library.rawValue, "library")
        XCTAssertEqual(OnboardingInitialTab.browse.rawValue, "browse")
    }
}

// MARK: - View model behavior

@MainActor
final class OnboardingFlowViewModelTests: XCTestCase {

    func testStartsAtWelcome() {
        let (defaults, suite) = makeDefaults()
        defer { defaults.removePersistentDomain(forName: suite) }
        let vm = OnboardingFlowViewModel(defaults: defaults)
        XCTAssertEqual(vm.currentStep, .welcome)
    }

    func testContinueFromWelcomeAdvancesToValueProp() {
        let (defaults, suite) = makeDefaults()
        defer { defaults.removePersistentDomain(forName: suite) }
        let vm = OnboardingFlowViewModel(defaults: defaults)

        vm.continueTapped()

        XCTAssertEqual(vm.currentStep, .valueProp)
        XCTAssertFalse(
            defaults.bool(forKey: OnboardingFlags.hasCompletedOnboardingKey),
            "Continue from welcome must not finish onboarding."
        )
    }

    func testContinueFromValuePropAdvancesToSearchPrompt() {
        let (defaults, suite) = makeDefaults()
        defer { defaults.removePersistentDomain(forName: suite) }
        let vm = OnboardingFlowViewModel(defaults: defaults, initialStep: .valueProp)

        vm.continueTapped()

        XCTAssertEqual(vm.currentStep, .searchPrompt)
        XCTAssertFalse(
            defaults.bool(forKey: OnboardingFlags.hasCompletedOnboardingKey),
            "Continue from value-prop must not finish onboarding."
        )
    }

    func testContinueFromSearchPromptFinishesAndRequestsBrowseTab() {
        // Defensive: even if the UI binds searchPrompt's primary CTA to
        // `continueTapped` instead of `getStartedTapped`, the terminal
        // step must still finish the flow and request Browse.
        let (defaults, suite) = makeDefaults()
        defer { defaults.removePersistentDomain(forName: suite) }
        let vm = OnboardingFlowViewModel(defaults: defaults, initialStep: .searchPrompt)

        vm.continueTapped()

        XCTAssertTrue(defaults.bool(forKey: OnboardingFlags.hasCompletedOnboardingKey))
        XCTAssertEqual(
            defaults.string(forKey: OnboardingFlags.requestedInitialTabKey),
            OnboardingInitialTab.browse.rawValue
        )
    }

    func testGetStartedFlipsFlagAndRequestsBrowseTab() {
        let (defaults, suite) = makeDefaults()
        defer { defaults.removePersistentDomain(forName: suite) }
        let vm = OnboardingFlowViewModel(defaults: defaults, initialStep: .searchPrompt)

        vm.getStartedTapped()

        XCTAssertTrue(
            defaults.bool(forKey: OnboardingFlags.hasCompletedOnboardingKey),
            "Get Started on search-prompt must mark onboarding complete."
        )
        XCTAssertEqual(
            defaults.string(forKey: OnboardingFlags.requestedInitialTabKey),
            OnboardingInitialTab.browse.rawValue,
            "Get Started must request the Browse tab."
        )
    }

    func testSkipFromWelcomeFlipsFlagAndDoesNotRequestTab() {
        let (defaults, suite) = makeDefaults()
        defer { defaults.removePersistentDomain(forName: suite) }
        let vm = OnboardingFlowViewModel(defaults: defaults)

        vm.skipTapped()

        XCTAssertTrue(defaults.bool(forKey: OnboardingFlags.hasCompletedOnboardingKey))
        XCTAssertNil(
            defaults.string(forKey: OnboardingFlags.requestedInitialTabKey),
            "Skip is the returning-user escape hatch; it must not preselect a tab."
        )
    }

    func testSkipFromValuePropFlipsFlag() {
        let (defaults, suite) = makeDefaults()
        defer { defaults.removePersistentDomain(forName: suite) }
        let vm = OnboardingFlowViewModel(defaults: defaults, initialStep: .valueProp)

        vm.skipTapped()

        XCTAssertTrue(defaults.bool(forKey: OnboardingFlags.hasCompletedOnboardingKey))
    }

    func testSkipFromSearchPromptFlipsFlag() {
        let (defaults, suite) = makeDefaults()
        defer { defaults.removePersistentDomain(forName: suite) }
        let vm = OnboardingFlowViewModel(defaults: defaults, initialStep: .searchPrompt)

        vm.skipTapped()

        XCTAssertTrue(defaults.bool(forKey: OnboardingFlags.hasCompletedOnboardingKey))
    }

    func testFlagSurvivesRelaunchSimulation() {
        // After Skip or Get Started, a fresh UserDefaults handle over
        // the same suite reads back the persisted true. This is the
        // relaunch contract.
        let (defaults, suite) = makeDefaults()
        defer { defaults.removePersistentDomain(forName: suite) }
        let vm = OnboardingFlowViewModel(defaults: defaults)

        vm.skipTapped()

        let reopened = UserDefaults(suiteName: suite)!
        XCTAssertTrue(
            reopened.bool(forKey: OnboardingFlags.hasCompletedOnboardingKey),
            "Onboarding-completed flag must survive a UserDefaults reopen."
        )
    }
}

// MARK: - ContentView tab-hint consumption

@MainActor
final class ContentViewInitialTabHintTests: XCTestCase {

    func testNoHintFallsBackToLibrary() {
        let (defaults, suite) = makeDefaults()
        defer { defaults.removePersistentDomain(forName: suite) }

        XCTAssertEqual(
            ContentView.consumeInitialTabHint(defaults: defaults),
            .library,
            "Without a hint, ContentView opens to Library."
        )
    }

    func testBrowseHintConsumedExactlyOnce() {
        let (defaults, suite) = makeDefaults()
        defer { defaults.removePersistentDomain(forName: suite) }

        defaults.set(
            OnboardingInitialTab.browse.rawValue,
            forKey: OnboardingFlags.requestedInitialTabKey
        )

        XCTAssertEqual(
            ContentView.consumeInitialTabHint(defaults: defaults),
            .browse,
            "First read must honor the hint."
        )
        XCTAssertEqual(
            ContentView.consumeInitialTabHint(defaults: defaults),
            .library,
            "Second read must fall back to Library — the hint is one-shot."
        )
        XCTAssertNil(
            defaults.string(forKey: OnboardingFlags.requestedInitialTabKey),
            "Hint must be cleared from defaults after consumption."
        )
    }

    func testEndToEndGetStartedHandsOffBrowseToContentView() {
        // Integration of the two halves: vm writes the hint, ContentView
        // reads-and-clears it. This pins the contract end-to-end.
        let (defaults, suite) = makeDefaults()
        defer { defaults.removePersistentDomain(forName: suite) }

        let vm = OnboardingFlowViewModel(defaults: defaults, initialStep: .searchPrompt)
        vm.getStartedTapped()

        let tab = ContentView.consumeInitialTabHint(defaults: defaults)
        XCTAssertEqual(tab, .browse)

        // And the second read must NOT still be Browse.
        XCTAssertEqual(
            ContentView.consumeInitialTabHint(defaults: defaults),
            .library
        )
    }

    func testSkipDoesNotPreselectBrowse() {
        // Skip is the returning-user escape hatch — they should land on
        // Library, not Browse.
        let (defaults, suite) = makeDefaults()
        defer { defaults.removePersistentDomain(forName: suite) }

        let vm = OnboardingFlowViewModel(defaults: defaults)
        vm.skipTapped()

        XCTAssertEqual(
            ContentView.consumeInitialTabHint(defaults: defaults),
            .library
        )
    }

    func testUnknownHintRawValueFallsBackToLibrary() {
        // Defensive: a future tab raw value that doesn't decode (e.g.
        // the string was persisted by a newer build, then the user
        // downgraded) must not crash — fall back to Library.
        let (defaults, suite) = makeDefaults()
        defer { defaults.removePersistentDomain(forName: suite) }

        defaults.set("settings", forKey: OnboardingFlags.requestedInitialTabKey)
        XCTAssertEqual(
            ContentView.consumeInitialTabHint(defaults: defaults),
            .library
        )
    }
}

// MARK: - View construction smoke tests

@MainActor
final class OnboardingViewSmokeTests: XCTestCase {

    func testOnboardingViewConstructs() {
        let view = OnboardingView()
        _ = view.body
    }
}
